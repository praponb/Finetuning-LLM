from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("opened_date") >= "2025-10-10")
    .select(
        "id",
        "city",
        "opened_date",
        F.row_number().over(w).alias("opened_date_row"),
    )
)
