from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "brand",
        "supplier_id",
        "launch_date",
        F.row_number().over(w).alias("launch_date_row"),
    )
)
