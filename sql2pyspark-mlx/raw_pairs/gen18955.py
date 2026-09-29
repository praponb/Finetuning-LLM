from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc())
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.max("quantity").alias("max_order_item_quantity"))

scored = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "FedEx", "Flash"]))
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.cume_dist().over(w2).alias("cost_cume_dist"),
    )
)

result = (
    scored
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "seq_no",
        "cost_cume_dist",
        F.col("max_order_item_quantity"),
    )
    .orderBy(F.col("max_order_item_quantity").desc_nulls_first())
)
