from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("claim_status", "holder_id").orderBy(F.col("claim_claimed_amount").desc(), F.col("id").desc(), F.col("claim_id").desc())
w_claim_type_by_claim_date_tie = Window.partitionBy("claim_type").orderBy("claim_date")
w_claim_type_status = Window.partitionBy("claim_type", "status")
w_status_by_claim_claimed_amou = Window.partitionBy("status").orderBy("claim_claimed_amount")

filtered_policys = (
    df_policies
    .filter((F.col("holder_id") >= 8) & (F.col("end_date") < F.lit("2026-03-13").cast("date")))
)

exploded_detail_claims = (
    filtered_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.status",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.status").alias("claim_status"),
        "c.claim_date",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

unique_rows = (
    exploded_detail_claims
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

windowed = (
    unique_rows
    .select(
        "id",
        "holder_id",
        "status",
        "claim_id",
        "claim_type",
        "claim_status",
        "claim_date",
        "claim_claimed_amount",
        F.rank().over(w_claim_type_by_claim_date_tie).alias("claim_date_rank"),
        F.size(F.collect_set("claim_id").over(w_claim_type_status)).alias("distinct_claim_id_in_claim_type_status"),
        F.dense_rank().over(w_status_by_claim_claimed_amou).alias("claim_claimed_amount_dense_rank"),
        F.round(F.col("claim_claimed_amount") - F.avg("claim_claimed_amount").over(w_claim_type_status), 2).alias("claim_claimed_amount_vs_claim_type_status_avg"),
    )
)

result = (
    windowed
    .groupingSets(
        [["holder_id", "claim_status"], ["holder_id"], ["claim_status"], []],
        "holder_id", "claim_status",
    )
    .agg(
        F.count(F.when(F.col("claim_type").isin("medical", "theft", "flood"), 1)).alias("n_claim_type_in_medical_theft"),
        F.sum("claim_claimed_amount_vs_claim_type_status_avg").alias("total_claim_claimed_amount_vs_claim_type_status_"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("holder_id").asc_nulls_last(),
        F.col("claim_status").asc_nulls_last(),
    )
)
