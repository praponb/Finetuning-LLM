from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter((F.col("city") == "Tokyo") & ((F.lower(F.trim("name")) == "carol diaz") | F.col("email").isNotNull()))
)

agg = (
    filtered_customers
    .groupBy("country", F.year("signup_date").alias("signup_year"))
    .agg(F.sort_array(F.collect_set("segment")).alias("segment_list"))
)

result = (
    agg
    .orderBy("country", F.col("signup_year").desc())
    .limit(1)
)
