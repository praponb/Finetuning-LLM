from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(F.hour("paid_at").between(10, 16) & (F.to_date("paid_at") < "2025-01-01")))
    .groupBy("method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
)
