from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter(F.col("product") == "HEALTH")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

with_parent = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.upper(F.trim(F.col("p.holder_name"))).like("D%"))
    .select("s.*", "p.holder_name")
)

result = (
    with_parent
    .groupingSets(
        [["holder_id", "status", "product"], ["holder_id"], ["status"], []],
        "holder_id", "status", "product",
    )
    .agg(
        F.round(F.avg("premium"), 2).alias("avg_premium"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
    )
)
