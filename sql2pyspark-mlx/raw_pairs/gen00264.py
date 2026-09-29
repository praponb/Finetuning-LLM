from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("opened_date"), "customer_id").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "opened_date",
        "customer_id",
        F.lead("opened_date", 1).over(w).alias("next_opened_date"),
    )
)
