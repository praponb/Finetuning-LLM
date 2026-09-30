from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("product_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country")
w_part = Window.orderBy("running_count_rating", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("product_category").orderBy(F.col("last4_avg_running_count_rating").desc_nulls_last(), "id")

with_latest = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost").alias("product_cost"))
)

enriched = (
    with_latest
    .select(
        "id",
        "supplier_name",
        "rating",
        "product_cost",
        F.sum("rating").over(w2).alias("running_sum_rating"),
        F.count("rating").over(w2).alias("running_count_rating"),
        F.min("rating").over(w2).alias("running_min_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w3)), 4).alias("rating_share"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.avg("running_count_rating").over(w_part).alias("last4_avg_running_count_rating"),
        F.sum("running_count_rating").over(w_part).alias("last4_sum_running_count_rating"),
        F.min("running_count_rating").over(w_part).alias("last4_min_running_count_rating"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.rating",
        "s.running_sum_rating",
        "s.running_count_rating",
        "s.running_min_rating",
        "s.rating_share",
        "s.last4_avg_running_count_rating",
        F.col("c.category"),
        F.col("c.brand").alias("product_brand"),
    )
)

cleaned = (
    with_last
    .filter((~F.col("product_brand").eqNullSafe(F.lit("Initech"))) & (F.col("running_min_rating") < 3.5))
    .select(
        "*",
        F.when(F.col("running_min_rating") >= 3.6, "High").when(F.col("running_min_rating") >= 1.7, "Medium").otherwise("Low").alias("running_min_rating_tier"),
        F.when(F.col("last4_avg_running_count_rating") >= 12.6, "High").when(F.col("last4_avg_running_count_rating") >= 7.0, "Medium").otherwise("Low").alias("last4_avg_running_count_rating_level"),
    )
)

with_last_suppliers = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.category").alias("product_category"),
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

deduped = (
    with_last_suppliers
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .groupBy("product_brand")
    .pivot("product_category", ["Sports", "Fashion"])
    .agg(F.avg("running_count_rating").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
