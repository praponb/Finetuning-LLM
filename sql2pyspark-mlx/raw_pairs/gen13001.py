from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("name_length").asc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("country").orderBy(F.col("uk_rows").desc())

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-04-06").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        F.length("name").alias("name_length"),
    )
)

valid_customers = (
    valid
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-09-07"))) & (F.col("segment").eqNullSafe(F.lit("Consumer"))))
    .select(
        "id",
        "email",
        "country",
        "segment",
        "signup_date",
        "credit_limit",
        "name_length",
        (F.col("credit_limit") + F.col("name_length")).alias("credit_limit_plus_name_length"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
    )
)

ranked = (
    valid_customers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "country", "segment", "name_length")
)

latest_match = (
    ranked
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"), F.col("c.balance"))
)

base = (
    latest_match
    .filter(F.col("segment") == "Consumer")
    .select(
        "id",
        "country",
        "segment",
        "status",
        "balance",
        F.coalesce("status", F.lit("unassigned")).alias("status_filled"),
    )
)

summary = (
    base
    .groupBy(
        "country",
        "segment",
        F.when(F.col("balance") >= 82000, "LARGE").otherwise("SMALL").alias("balance_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_rows"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("balance_per_sg"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_rows"),
    )
    .withColumn("uk_rows_dense_rank", F.dense_rank().over(w3))
)

result = (
    summary
    .select(
        "country",
        "segment",
        "balance_tier",
        "num_rows",
        "balance_per_sg",
        "smb_rows",
        "uk_rows_dense_rank",
        F.when(F.col("segment").rlike("^(Corporate|Consumer)$"), "EXTENDED").when(F.col("segment").rlike("^(SMB)$"), "GROUP_A").otherwise("PRIORITY").alias("segment_zone"),
    )
)
