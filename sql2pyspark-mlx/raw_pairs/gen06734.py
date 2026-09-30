from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")

metrics = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-09-01", "2023-10-01"))
    .withColumn("first_known_monthly_fee", F.first("monthly_fee", ignorenulls=True).over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w2))
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        "first_known_monthly_fee",
        "row_seq",
    )
)
