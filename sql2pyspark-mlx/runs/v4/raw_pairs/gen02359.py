from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("num_items").desc(), "id")

flattened_attrs = (
    df_carts
    .filter(F.hour("created_at").between(8, 13))
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        "attributes",
        F.size("items").alias("num_items"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    flattened_attrs
    .withColumn("running_num_items", F.sum("num_items").over(w))
    .withColumn("num_items_max_2h", F.max("num_items").over(w2))
    .withColumn("num_items_bucket", F.ntile(3).over(w3))
)
