from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w_ordered = Window.partitionBy(F.year("signup_date"), "segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-08-04").cast("date"))
    .withColumn("clean_name", F.trim("name"))
    .withColumn("credit_limit_filled", F.coalesce("credit_limit", F.lit(0)))
    .withColumn("resolved_segment", F.coalesce("segment", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .select(
        "id",
        "name",
        "segment",
        "signup_date",
        "age",
        "clean_name",
        "resolved_segment",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4).alias("credit_limit_share"),
        F.sum("credit_limit_filled").over(w_ordered).alias("cumulative_credit_limit_filled"),
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_6"),
    )
)
