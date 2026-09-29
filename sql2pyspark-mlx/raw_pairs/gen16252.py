from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("started_at"), "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.to_date("started_at"), "customer_id").orderBy("started_at", "id")
w3 = Window.orderBy("started_at", "id")

valid = (
    df_sessions
    .filter(F.col("country").isin(["UK", "US"]))
    .select(
        "*",
        F.concat_ws(",", "browser", "country").alias("browser_country_key"),
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
    )
)

result = (
    valid
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "started_at_label",
        F.first("browser", ignorenulls=True).over(w).alias("first_known_browser"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("row_seq_2"),
    )
)
