from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("policy_product").orderBy("joined_on", "id", "policy_id")

scoped_holders = (
    df_policyholders
    .filter(F.col("risk_score") > 0.1)
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "segment",
    )
)

line_level = (
    scoped_holders.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_name",
        "s.joined_on",
        F.col("p.id").alias("policy_id"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.product").alias("policy_product"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

latest_per_key = (
    line_level
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .cube("policy_id", "policy_product")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.stddev("policy_premium"), 2).alias("stddev_policy_premium"),
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("policy_product").asc_nulls_last(),
    )
)
