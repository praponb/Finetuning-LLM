from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id")

latest = (
    df_inventory
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .groupBy(
        F.when(F.col("quantity") >= 270, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
    )
    .filter(F.col("min_quantity") >= 0)
)
