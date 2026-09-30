from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .rollup("customer_id", F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list_2"),
    )
)
