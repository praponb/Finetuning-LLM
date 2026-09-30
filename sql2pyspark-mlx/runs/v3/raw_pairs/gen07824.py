from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("brand") == "Globex")
    .select(
        "id",
        "brand",
        "cost",
        "supplier_id",
        "launch_date",
        F.row_number().over(w).alias("cost_row"),
        F.row_number().over(w2).alias("launch_date_row"),
    )
)
