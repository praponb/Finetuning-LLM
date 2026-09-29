from pyspark.sql import functions as F

recent_policys = df_policies.filter(F.col("premium") > 2013.8)

joined = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.region").alias("holder_region"),
        "p.holder_name",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

result = (
    joined
    .filter(F.col("holder_id") >= 13)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "holder_name",
        "holder_joined_on",
    )
    .orderBy(F.col("premium").asc_nulls_last())
)
