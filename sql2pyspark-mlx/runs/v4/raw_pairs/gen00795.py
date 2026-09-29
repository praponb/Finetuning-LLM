from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "product_id",
        "units",
        F.row_number().over(w).alias("sale_date_row"),
        F.lead("units", 1).over(w2).alias("next_units"),
    )
)
