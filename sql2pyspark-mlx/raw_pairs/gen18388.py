from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("started_at"), "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser", "device").orderBy(F.col("started_at").desc(), "id")
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("amount").alias("min_order_amount"))

metrics = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.last("browser", ignorenulls=True).over(w).alias("last_known_browser"),
        F.last("country", ignorenulls=True).over(w_hist).alias("last_known_country"),
        F.first("country", ignorenulls=True).over(w_running).alias("first_known_country"),
        F.last("browser", ignorenulls=True).over(w_hist).alias("last_known_browser_2"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

result = (
    metrics
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "last_known_browser_2",
        "seq_no",
        F.when(F.col("country").rlike("^(DE|TH)$"), "PRIORITY").when(F.col("country").rlike("^(US|SG)$"), "OTHER_ZONE").otherwise("CORE").alias("country_zone"),
        F.col("min_order_amount"),
    )
    .orderBy("min_order_amount")
)
