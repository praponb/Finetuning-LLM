from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc())
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.min("quantity").alias("min_inventory_quantity"))

ranked = (
    df_store_sales
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        "latest_rank",
        F.col("min_inventory_quantity"),
    )
    .orderBy(F.col("revenue").desc_nulls_last(), "min_inventory_quantity")
)
