from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("sale_date"), "product_id")

result = (
    df_store_sales
    .filter(F.col("revenue") >= 1700)
    .select(
        "id",
        "sale_date",
        "product_id",
        "units",
        F.sum("units").over(w).alias("sale_date_year_sum_units"),
    )
)
