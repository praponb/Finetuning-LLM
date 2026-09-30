from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("holder_region").orderBy(F.col("holder_risk_score").desc(), F.col("id").desc())

cleaned_policys = (
    df_policies
    .filter((F.col("premium").between(1042.5, 3532.7)) & (F.col("status") == "ACTIVE"))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.concat_ws("-", F.col("status"), F.col("product")).alias("status_product_label"),
        F.round(F.col("premium") * 1.07, 2).alias("premium_adj"),
    )
)

combined_details_holder = (
    cleaned_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.premium_bucket",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
    )
)

latest_per_key = (
    combined_details_holder
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

without_matches = (
    latest_per_key
    .crossJoin(latest_per_key.agg(F.avg("holder_risk_score").alias("overall_avg_holder_risk_score")))
    .filter(F.col("holder_risk_score") > F.col("overall_avg_holder_risk_score"))
    .drop("overall_avg_holder_risk_score")
)

result = (
    without_matches
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "premium_bucket",
        "holder_risk_score",
        "holder_region",
    )
    .orderBy(
        F.col("holder_region").asc_nulls_last(),
        F.col("premium_bucket").desc(),
        "id",
    )
    .limit(10)
)
