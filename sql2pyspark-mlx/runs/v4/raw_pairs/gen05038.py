from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "status",
        "customer_id",
        "opened_date",
        F.ntile(3).over(w).alias("quartile"),
    )
    .orderBy("id")
)
