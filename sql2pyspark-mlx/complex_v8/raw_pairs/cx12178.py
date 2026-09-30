from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("holder_id", "status").orderBy(F.col("start_date").desc(), F.col("id").desc())

prepared_policys = (
    df_policies
    .filter((F.col("status").isNotNull()) | (F.col("start_date") < F.lit("2025-05-30").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

latest = (
    prepared_policys
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

without_matches = (
    latest.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_date").isNotNull()), "left_anti")
    .filter(F.col("product") == "TRAVEL")
)

result = (
    without_matches
    .filter(F.col("start_date").between(F.lit("2024-10-22").cast("date"), F.lit("2025-08-24").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
    )
    .orderBy(F.col("product").desc(), "id")
    .limit(20)
)
