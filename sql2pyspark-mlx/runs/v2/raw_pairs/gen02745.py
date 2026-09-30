from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.to_date("paid_at") < "2025-09-23")
    .select("id", "paid_at", F.ntile(10).over(w).alias("quartile"))
)
