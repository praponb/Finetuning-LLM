from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~((F.lower(F.trim("name")) == "henry ford") | F.col("is_active")))
    .groupBy(
        F.when(F.col("country").isin(["TH", "US"]), "tier_1").otherwise("other").alias("country_group"),
        F.when(F.col("age") >= 61, "high").when(F.col("age") >= 32, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.count("*").alias("num_customers"))
    .orderBy(F.col("age_bucket").desc(), "num_customers")
)
