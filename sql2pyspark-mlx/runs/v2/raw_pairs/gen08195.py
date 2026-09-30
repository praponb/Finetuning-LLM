from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "units",
        "sale_date",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "product_id", "units", "sale_date")
    .orderBy(F.col("product_id").desc())
)
