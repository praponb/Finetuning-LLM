from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy(F.col("opened_date").desc_nulls_first(), "id")

latest = (
    df_accounts
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "opened_date")
)

result = (
    latest
    .withColumn("year_start", F.date_trunc("YEAR", "opened_date"))
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date"))
    .rollup("year_start", "activity_month")
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("row_count").desc_nulls_last())
)
