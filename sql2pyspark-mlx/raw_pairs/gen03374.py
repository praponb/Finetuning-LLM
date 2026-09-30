from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")

deduped = (
    df_accounts
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "account_type", "status")
)

joined = (
    deduped
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.id", "e.customer_id", "e.status", F.col("p.name"), F.col("p.age"))
)

base = (
    joined
    .filter(F.col("status").isNotNull() & (F.col("status").eqNullSafe(F.lit("CLOSED"))))
    .select(
        "id",
        "customer_id",
        "name",
        "age",
        F.initcap(F.trim("name")).alias("name_title"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

kept_keys = (
    base
    .select("customer_id")
    .intersect(
        df_accounts
        .filter(F.col("balance") < 73000)
        .select("customer_id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .alias("s")
    .join(df_customers.alias("dim"), F.col("s.customer_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["city", "country"], ["city"], []],
        F.col("dim.city").alias("city"),
        F.col("dim.country").alias("country"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("s.age")).alias("avg_age"),
    )
)
