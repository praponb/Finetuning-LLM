from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .withColumn("first_known_attr_key", F.first("attr_key", ignorenulls=True).over(w))
    .withColumn("first_known_attr_key_2", F.first("attr_key", ignorenulls=True).over(w_running))
)
