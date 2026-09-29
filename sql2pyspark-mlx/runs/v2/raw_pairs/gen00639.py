from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0))) / F.count("*")), 1).alias("pct_smb"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
    .filter(F.col("pct_smb") >= 59)
    .select("city", "pct_smb", "city_values")
)
