from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)
