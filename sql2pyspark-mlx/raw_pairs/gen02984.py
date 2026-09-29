from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "k").orderBy(F.col("created_at").desc(), "id", "k")
w2 = Window.orderBy("created_at", "id", "k")
w3 = Window.partitionBy("k").orderBy("created_at", "id", "k")
w4 = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "k",
        "attr_value",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("event_seq_2"),
        F.last("k", ignorenulls=True).over(w4).alias("last_known_k"),
    )
)
