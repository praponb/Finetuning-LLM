from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("start_date"), "plan").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "start_date",
        "plan",
        "monthly_fee",
        F.row_number().over(w).alias("monthly_fee_row"),
    )
    .orderBy(F.col("monthly_fee").desc())
)
