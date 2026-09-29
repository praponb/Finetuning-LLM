from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["London", "Singapore", "New York"]) & F.col("city").isin(["Berlin", "Singapore", "Bangkok"]))
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.countDistinct("credit_limit").alias("unique_citys"),
    )
    .orderBy(F.col("signup_year").desc())
)
