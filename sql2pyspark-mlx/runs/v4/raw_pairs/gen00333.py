from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("credit_limit", "id")

enriched = (
    df_customers
    .withColumn("running_age", F.sum("age").over(w))
    .withColumn("credit_limit_bucket", F.ntile(5).over(w2))
)

result = (
    enriched
    .groupBy(
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
        F.when(F.col("age") >= 61, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
        F.when(F.col("credit_limit") >= 14000, "LARGE").otherwise("SMALL").alias("credit_limit_tier"),
    )
    .agg(
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.bit_or("age").alias("age_bit_or"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .orderBy(F.col("credit_limit_tier").desc())
)
