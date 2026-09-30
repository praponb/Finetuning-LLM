from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount", "id")
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.sum("units").alias("sum_store_sale_units"))

ranked = (
    df_orders
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "customer_id", "store_id", "order_date", "status", "discount")
)

result = (
    ranked
    .join(units_by_store_id, on="store_id", how="left")
    .select("id", "store_id", F.col("sum_store_sale_units"))
    .orderBy(F.col("store_id").desc())
)
