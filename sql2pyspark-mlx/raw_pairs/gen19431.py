from pyspark.sql import functions as F

matched_keys = (
    df_customers
    .select("segment", "id")
    .intersectAll(
        df_customers
        .filter(F.col("credit_limit") <= 5100)
        .select("segment", "id")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("week_start", F.date_trunc("WEEK", "signup_date"))
    .withColumn("age_tier", F.when(F.col("age") >= 71, "LARGE").otherwise("SMALL"))
    .cube("week_start", "age_tier")
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count("*").alias("num_rows"),
        F.grouping("week_start").alias("gid"),
    )
    .orderBy(F.col("num_rows").desc())
)
