from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "TH")
    .groupBy(F.year("signup_date").alias("signup_year"), "is_active")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
)
