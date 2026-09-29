from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("segment").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("dedup_rank", "id").rowsBetween(-2, -1)

deduped = (
    df_customers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped
    .withColumn("last_known_age", F.last("age", ignorenulls=True).over(w2))
    .withColumn("age_avg_2_prior", F.avg("age").over(w3))
)

matched_keys = (
    scored
    .select("id", "country")
    .intersect(
        df_customers
        .filter(F.col("segment").isin(["Consumer", "SMB"]))
        .select("id", "country")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter((F.col("signup_date") >= F.lit("2023-10-13").cast("date")) & F.col("city").isNotNull() & (F.col("last_known_age") > 51))
    .select(
        "*",
        F.floor("credit_limit").alias("credit_limit_whole"),
        (F.col("credit_limit") + F.col("age_avg_2_prior")).alias("credit_limit_plus_age_avg_2_prior"),
    )
)

result = (
    filtered
    .select(
        "id",
        "name",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "age",
        "dedup_rank",
        "last_known_age",
    )
    .orderBy("country", "signup_date")
)
