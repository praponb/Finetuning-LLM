from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "units",
        (F.col("units") - F.lag("units", 1, 0).over(w)).alias("units_change"),
    )
)
