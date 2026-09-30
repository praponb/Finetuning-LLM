from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("sale_date"), "store_id").orderBy("units", "id")

result = (
    df_store_sales
    .filter(F.col("product_id") == 4)
    .select(
        "id",
        "sale_date",
        "store_id",
        "units",
        F.row_number().over(w).alias("units_row"),
    )
)
