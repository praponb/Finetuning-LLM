from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Tokyo", "Chiang Mai"]) & F.col("name").like("Hen%") & (F.col("credit_limit") > 12000))
    .groupBy(F.month("signup_date").alias("signup_month"), "segment", "country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
    )
)
