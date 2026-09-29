from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id")

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "revenue",
        F.lead("units").over(w).alias("next_units"),
        F.lead("revenue").over(w).alias("next_revenue"),
        F.row_number().over(w).alias("row_seq"),
    )
)
