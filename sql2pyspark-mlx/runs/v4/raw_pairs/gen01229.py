from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("name").like("%Chen%") & F.col("age").between(36, 40) & F.col("email").like("%@gmail.com"))
)

agg = (
    base
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.max("signup_date").alias("last_signup_date"),
    )
)

result = agg.orderBy(F.col("signup_year").desc())
