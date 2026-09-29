from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("claim_date").desc(), F.col("id").desc())

in_window = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.status",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

derived = (
    in_window
    .select(
        "*",
        (F.col("premium") + F.col("claimed_amount")).alias("premium_plus_claimed_amount"),
        F.when(F.col("premium") >= 3719.7, "A").when(F.col("premium") >= 2033.8, "B").otherwise("C").alias("premium_tier"),
        F.date_trunc("year", F.col("claim_date")).alias("claim_date_year"),
    )
)

unique_rows = (
    derived
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .groupingSets(
        [["product", "status"], ["product"], ["status"], []],
        "product", "status",
    )
    .agg(F.countDistinct("status").alias("distinct_status"))
    .orderBy(F.col("product").asc_nulls_last(), F.col("status").asc_nulls_last())
)
