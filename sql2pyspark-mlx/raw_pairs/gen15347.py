from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("sale_date"), "store_id")

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "store_id",
        "revenue",
        F.sum("revenue").over(w).alias("sale_date_year_sum_revenue"),
    )
)
