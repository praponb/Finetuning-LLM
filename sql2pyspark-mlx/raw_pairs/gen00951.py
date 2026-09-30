from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([5, 10, 12]) & F.col("sqft").between(7057, 13799))
    .withColumn("store_name_slug", F.regexp_replace("store_name", " ", "-"))
)
