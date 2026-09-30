from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2024-12-08", "2025-03-01"))
    .select(
        "id",
        "status",
        "end_date",
        "monthly_fee",
        F.lead("monthly_fee", 2, 0).over(w).alias("next_monthly_fee"),
    )
)
