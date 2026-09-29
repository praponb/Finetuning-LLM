from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("customer_id").orderBy("weight_kg", "id").rowsBetween(-6, Window.currentRow)
w_running_2 = Window.partitionBy("customer_id")

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        "attributes",
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("direct")).alias("utm_source_or_default"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.size("items").alias("num_items"),
    )
)

result = (
    parsed
    .select(
        "id",
        "max_item_price",
        "bulk_lines",
        "utm_source_or_default",
        "weight_kg",
        "num_items",
        F.min("weight_kg").over(w_running).alias("last7_min_weight_kg"),
        F.avg("weight_kg").over(w_running).alias("last7_avg_weight_kg"),
        F.max("weight_kg").over(w_running).alias("last7_max_weight_kg"),
        F.count("num_items").over(w_running_2).alias("customer_id_count_num_items"),
    )
)
