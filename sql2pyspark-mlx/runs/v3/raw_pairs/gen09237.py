from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.hour("paid_at").between(11, 19) & (F.col("status") == "REFUNDED"))
)

agg = (
    base
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
)

result = agg
