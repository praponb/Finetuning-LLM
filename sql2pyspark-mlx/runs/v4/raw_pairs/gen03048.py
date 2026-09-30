from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("segment", "city").orderBy(F.col("credit_limit").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2024-02-13").cast("date")) & F.col("segment").isNotNull())
    .withColumn("country_title", F.initcap(F.trim("country")))
    .withColumn("signup_date_label", F.date_format("signup_date", "yyyy-MM-dd"))
    .withColumn("segment_filled", F.coalesce("segment", F.lit("NONE")))
)

enriched = (
    filtered
    .select(
        "*",
        F.corr("credit_limit", "age").over(w).alias("corr_credit_limit_age"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.channel").alias("order_channel"), F.col("c.discount"))
)

filtered_customers = (
    with_last
    .filter(F.col("age") == 63)
    .withColumn("email_length", F.length("email"))
)

result = (
    filtered_customers
    .groupBy("order_channel")
    .pivot("country", ["SG", "TH"])
    .agg(F.max("row_seq").alias("value_sum"))
)
