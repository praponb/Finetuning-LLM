from pyspark.sql import functions as F

recent_customers = df_customers.filter(F.col("name").like("O%"))

summary = (
    recent_customers
    .groupBy("segment", F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.sum("age").alias("total_age"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)

result = (
    summary
    .filter(F.col("total_age") > 180)
    .select("segment", "signup_quarter", "total_age")
    .orderBy(F.col("signup_quarter").desc(), F.col("total_age").desc())
)
