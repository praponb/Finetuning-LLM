from pyspark.sql import functions as F

dim_totals = (
    df_accounts
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["country", "status"], ["country"], []],
        F.col("p.country").alias("country"),
        F.col("a.status").alias("status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("a.balance")).alias("max_balance"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    dim_totals
    .select("country", "status", "num_rows", "gid")
    .orderBy(F.col("num_rows").asc_nulls_first(), "country")
)
