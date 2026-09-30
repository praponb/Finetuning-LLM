from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")

ranked = (
    df_store_sales
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "product_id", "units", "revenue")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("revenue") >= 3300, "HIGH").otherwise("LOW").alias("revenue_tier"),
        "product_id",
    )
    .agg(
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.bit_and("units").alias("units_bit_and"),
    )
    .orderBy(F.col("product_id").desc())
)
