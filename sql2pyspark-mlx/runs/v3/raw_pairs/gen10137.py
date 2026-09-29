from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("city") != "Bangkok") & F.month("signup_date").isin([5, 6, 12]) & F.col("city").isNotNull())
    .agg(
        F.max("signup_date").alias("latest_signup_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
    .orderBy(F.col("country_values").desc())
)
