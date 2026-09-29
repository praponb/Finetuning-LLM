from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("adjusted_rating").asc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("country") == "SG"))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.trim("supplier_name").alias("clean_supplier_name"),
        F.round((F.col("rating") * 1.07), 2).alias("adjusted_rating"),
    )
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

kept = (
    latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.brand").isin(["Initech", "Acme"])), "left_semi")
)

with_last = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.cost"), F.col("c.launch_date").alias("product_launch_date"))
)

result = (
    with_last
    .cube(
        F.when(F.col("rating") >= 4.2, "LARGE").otherwise("SMALL").alias("rating_tier"),
        "city",
    )
    .agg(
        F.count_if(F.col("country") == "TH").alias("th_events"),
        F.round(F.avg("adjusted_rating"), 2).alias("avg_adjusted_rating"),
        F.percentile_approx("adjusted_rating", [0.25, 0.5, 0.75]).alias("adjusted_rating_quartiles"),
        F.sum("adjusted_rating").alias("total_adjusted_rating"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
        F.sum(F.when(F.col("country") == "UK", F.col("adjusted_rating")).otherwise(0)).alias("uk_adjusted_rating"),
    )
    .orderBy(F.col("total_adjusted_rating").asc_nulls_last())
)
