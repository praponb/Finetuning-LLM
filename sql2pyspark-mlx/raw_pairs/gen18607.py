from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("num_attributes", "id")
w2 = Window.partitionBy(F.year("created_at")).orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy(F.to_date("created_at")).orderBy("num_attributes", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy(F.col("weight_kg").desc(), "id")

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.coalesce(F.col("attributes")["gift"], F.lit("none")).alias("gift_or_default"),
        F.size("attributes").alias("num_attributes"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.transform("items", lambda x: x["product_id"]).alias("product_ids"),
        F.size("items").alias("num_items"),
    )
)

result = (
    flattened_attrs
    .withColumn("next_num_attributes", F.lead("num_attributes", 1, 0).over(w))
    .withColumn("weight_kg_avg_2h", F.avg("weight_kg").over(w2))
    .withColumn("cumulative_weight_kg", F.sum("weight_kg").over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)
