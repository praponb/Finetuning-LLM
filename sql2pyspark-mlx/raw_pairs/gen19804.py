from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", "segment")
    .subtract(
        df_customers
        .filter(F.upper(F.trim("email")) == "ALICE@GMAIL.COM")
        .select("country", "segment")
    )
)
