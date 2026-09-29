from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country", "city").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city", "country").orderBy("launch_date", "id")
w4 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("product_price", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("city", "country").orderBy(F.col("last_known_rating").desc(), "id")

with_latest = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.rating",
        F.col("c.launch_date"),
        F.col("c.price").alias("product_price"),
    )
)

enriched = (
    with_latest
    .filter(F.col("rating") >= 2.7)
    .select(
        "*",
        F.last("rating", ignorenulls=True).over(w2).alias("last_known_rating"),
        F.lead("rating", 1).over(w3).alias("next_rating"),
        F.first("rating", ignorenulls=True).over(w4).alias("first_known_rating"),
    )
)

enriched_suppliers = (
    enriched
    .select(
        "*",
        F.avg("first_known_rating").over(w2).alias("running_avg_first_known_rating"),
        F.sum("first_known_rating").over(w2).alias("running_sum_first_known_rating"),
        F.max("first_known_rating").over(w2).alias("running_max_first_known_rating"),
        F.avg("next_rating").over(w5).alias("next_rating_avg_6_prior"),
    )
)

latest = (
    enriched_suppliers
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "launch_date",
        "last_known_rating",
        "next_rating",
        "running_max_first_known_rating",
        "next_rating_avg_6_prior",
    )
)

result = (
    latest
    .select(
        "id",
        "launch_date",
        "last_known_rating",
        "next_rating",
        "running_max_first_known_rating",
        "next_rating_avg_6_prior",
    )
    .orderBy(F.col("last_known_rating").desc(), F.col("next_rating_avg_6_prior").desc())
)
