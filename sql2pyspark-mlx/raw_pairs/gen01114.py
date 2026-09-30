from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("started_at")).orderBy("started_at", "id")
w_hist = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_sessions
    .filter(F.col("country").isin(["TH", "DE"]))
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
        F.row_number().over(w2).alias("event_seq"),
        F.first("country", ignorenulls=True).over(w_hist).alias("first_known_country"),
        F.last("country", ignorenulls=True).over(w4).alias("last_known_country_2"),
    )
)
