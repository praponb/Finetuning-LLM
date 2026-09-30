from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("rank_in_group", "id")

latest = (
    df_accounts
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.account_type",
        "e.status",
        "e.rank_in_group",
        F.col("p.country"),
    )
)

joined = (
    enriched
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.credit_limit") < 8100)
    .select("e.*", F.col("d.email"))
)

scored = joined.withColumn("row_seq", F.row_number().over(w2))

result = (
    scored
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.city").alias("city"), F.col("e.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.row_seq")).alias("max_row_seq"),
    )
)
