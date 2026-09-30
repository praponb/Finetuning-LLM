from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("adjuster_id", "product").orderBy("claim_date", "id")

covered = (
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
        "c.claim_type",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

latest_per_key = (
    covered
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

totals = (
    latest_per_key
    .groupBy(
        "policy_id",
        F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"),
    )
    .agg(
        F.count(F.when(F.col("claim_type").isNotNull(), 1)).alias("n_claim_type_is_not_null"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.round(100 * F.count(F.when(F.col("product") == "LIFE", 1)) / F.count("*"), 1).alias("pct_product_life"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("total_claimed_amount") > 6745.1)
)

result = (
    totals
    .select(
        "policy_id",
        "claim_date_ym",
        "total_claimed_amount",
        "pct_product_life",
    )
    .unpivot(
        ids=["policy_id", "claim_date_ym"],
        values=["total_claimed_amount", "pct_product_life"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
