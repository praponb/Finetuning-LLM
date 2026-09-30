from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("units", "id")
w2 = Window.partitionBy(F.year("sale_date"), "product_id")

result = (
    df_store_sales
    .filter(F.col("revenue") >= 2500)
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "sale_date",
        F.row_number().over(w).alias("units_row"),
        F.max("units").over(w2).alias("sale_date_year_max_units"),
    )
)
