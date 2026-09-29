from pyspark.sql import functions as F

kept_keys = (
    df_customers
    .select("segment")
    .intersect(
        df_customers
        .filter(F.length("email") >= 7)
        .select("segment")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("country")
    .agg(
        F.bit_and("age").alias("age_bit_and"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.count_if(F.col("segment") == "SMB").alias("smb_events"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
    .filter(F.col("age_bit_and") >= 1)
)
