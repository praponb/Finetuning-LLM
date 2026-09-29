from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter(~(~F.col("is_active") & ~F.col("name").like("Kan%") & F.col("city").isin(["New York", "Chiang Mai", "Berlin"])))
)

summary = (
    recent_customers
    .groupBy("segment")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
)

result = summary.select("segment", "country_list", "max_credit_limit")
